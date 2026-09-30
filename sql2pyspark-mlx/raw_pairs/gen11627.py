from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("shipped_date").desc_nulls_first(), "id")
w2 = Window.partitionBy("carrier").orderBy(F.col("cost").asc_nulls_first(), "id")
w3 = Window.partitionBy("carrier").orderBy(F.col("shipped_date_day").desc(), "id")
w4 = Window.partitionBy(F.month("shipped_date"), "carrier").orderBy(F.col("cost").desc())
w5 = Window.partitionBy("carrier").orderBy(F.col("shipped_date_day").desc_nulls_last())

cleaned = (
    df_shipments
    .filter((F.col("shipped_date") > F.lit("2023-07-16").cast("date")) & F.col("order_id").isin([2, 17, 20, 31]))
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.substring("status", 1, 3).alias("status_prefix"),
    )
)

ranked = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

base = (
    ranked
    .filter((F.col("delivered_date") >= F.lit("2023-08-02").cast("date")) & F.col("carrier").isNotNull())
    .select("*", F.dayofmonth("shipped_date").alias("shipped_date_day"))
)

deduped = (
    base
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status_prefix",
        "latest_rank",
        "shipped_date_day",
    )
)

metrics = (
    deduped
    .withColumn("shipped_date_day_bucket", F.ntile(10).over(w3))
    .withColumn("cost_rank", F.rank().over(w4))
    .withColumn("shipped_date_day_percent_rank", F.percent_rank().over(w5))
)

result = (
    metrics
    .select(
        "id",
        "shipped_date",
        "cost",
        "status_prefix",
        "latest_rank",
        "shipped_date_day_bucket",
        "cost_rank",
        "shipped_date_day_percent_rank",
    )
    .orderBy(F.col("shipped_date_day_percent_rank").desc_nulls_last())
)
