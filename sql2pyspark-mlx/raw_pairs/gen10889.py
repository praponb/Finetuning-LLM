from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("days_since_shipped_date").desc(), "id")
w2 = Window.partitionBy("carrier").orderBy(F.col("cost_filled").desc(), "id")

base = (
    df_shipments
    .filter(F.col("carrier").isNotNull())
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        F.datediff(F.current_date(), "shipped_date").alias("days_since_shipped_date"),
        F.concat("carrier", F.lit(" "), "status").alias("carrier_status"),
        F.coalesce("carrier", F.lit("N/A")).alias("carrier_clean"),
    )
)

valid = (
    base
    .filter(F.col("cost").between(30, 48) & F.col("carrier").isNotNull())
    .withColumn("cost_filled", F.coalesce("cost", F.lit(0)))
    .withColumn("carrier_clean_2", F.coalesce("carrier", F.lit("OTHER")))
)

latest = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "carrier", "carrier_status", "cost_filled")
)

latest_shipments = (
    latest
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

result = (
    latest_shipments
    .groupBy("carrier")
    .agg(
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("carrier") == "DHL", 1)).alias("dhl_count"),
        F.percentile_approx("cost_filled", 0.95).alias("cost_filled_p95"),
        F.sum(F.when(F.col("carrier") == "UPS", F.col("cost_filled")).otherwise(0)).alias("ups_cost_filled"),
        F.count(F.when(F.col("carrier") == "Flash", 1)).alias("flash_rows"),
    )
)
