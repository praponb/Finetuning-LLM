from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("delivered_date_plus_months").asc_nulls_last(), "id")
w2 = Window.partitionBy("status", "carrier").orderBy(F.col("shipped_date").desc(), "id")

filtered = (
    df_shipments
    .filter((F.col("delivered_date") > F.lit("2023-03-13").cast("date")) & (F.col("carrier").eqNullSafe(F.lit("UPS"))))
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.when(F.col("carrier").isin(["UPS", "DHL"]), "Group A").otherwise("Other").alias("carrier_group"),
        F.concat("status", F.lit(", "), "carrier").alias("status_carrier"),
        F.coalesce("carrier", F.lit("NONE")).alias("carrier_filled"),
    )
)

base = (
    filtered
    .filter(F.col("carrier") == "Kerry")
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "carrier_group",
        "status_carrier",
        "carrier_filled",
        F.add_months("delivered_date", 12).alias("delivered_date_plus_months"),
    )
)

deduped = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

latest = (
    deduped
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .groupBy(
        F.date_trunc("MONTH", "delivered_date_plus_months").alias("activity_month"),
    )
    .agg(
        F.count(F.when(F.col("status") == "DELIVERED", 1)).alias("delivered_rows"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
    )
    .orderBy(F.col("delivered_rows").desc_nulls_first())
)
