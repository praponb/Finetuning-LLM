from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("city").orderBy(F.col("sale_date").desc(), "id")
w2 = Window.partitionBy("store_id")

filtered = (
    df_store_sales
    .filter(F.col("revenue") > 2700)
    .select("*", F.date_trunc("WEEK", "sale_date").alias("sale_date_period"))
)

with_dims = (
    filtered
    .alias("s")
    .join(df_stores.alias("p2"), F.col("s.store_id") == F.col("p2.id"), "inner")
    .select("s.*", F.col("p2.opened_date"), F.col("p2.sqft"), F.col("p2.city"))
)

metrics = (
    with_dims
    .select(
        "*",
        F.row_number().over(w_recent).alias("event_seq"),
        F.stddev_samp("revenue").over(w2).alias("store_id_stddev_samp_revenue"),
    )
)

deduped = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w_recent))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    deduped
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "opened_date",
        "sqft",
        "store_id_stddev_samp_revenue",
    )
    .orderBy(F.col("id").desc())
    .limit(10)
)
