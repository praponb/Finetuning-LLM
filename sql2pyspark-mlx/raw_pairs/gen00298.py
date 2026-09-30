from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "carrier").orderBy(F.col("days_delivered_date_to_shipped_date").desc_nulls_first(), "id")
w2 = Window.partitionBy("carrier").orderBy("shipped_date", "id")

cleaned = (
    df_shipments
    .filter((F.col("shipped_date") >= F.lit("2023-03-11").cast("date")) & (~F.col("carrier").eqNullSafe(F.lit("FedEx"))))
    .select(
        "*",
        F.last_day("delivered_date").alias("delivered_date_month_end"),
        F.coalesce("carrier", F.lit("N/A")).alias("carrier_filled"),
    )
)

cleaned_shipments = (
    cleaned
    .filter(F.col("carrier").isNotNull())
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "carrier_filled",
        F.datediff("shipped_date", "delivered_date").alias("days_delivered_date_to_shipped_date"),
        F.datediff("delivered_date_month_end", "shipped_date").alias("days_shipped_date_to_delivered_date_month_end"),
        F.coalesce("carrier", F.lit("N/A")).alias("carrier_clean"),
    )
)

deduped = (
    cleaned_shipments
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    deduped
    .filter(F.col("carrier") != "DHL")
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "days_shipped_date_to_delivered_date_month_end",
        F.lead("cost").over(w2).alias("next_cost"),
        F.lag("days_delivered_date_to_shipped_date").over(w2).alias("prev_days_delivered_date_to_shipped_date"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

result = (
    enriched
    .withColumn("seq_no_tier", F.when(F.col("seq_no") >= 7, "HIGH").otherwise("LOW"))
    .rollup("seq_no_tier", "carrier")
    .agg(
        F.count("*").alias("row_count"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.sum(F.when(F.col("carrier") == "Flash", F.col("cost")).otherwise(0)).alias("flash_cost"),
        F.countDistinct("carrier").alias("distinct_carriers"),
        F.grouping("seq_no_tier").alias("gid"),
    )
    .filter(F.col("distinct_carriers") > 1)
)
