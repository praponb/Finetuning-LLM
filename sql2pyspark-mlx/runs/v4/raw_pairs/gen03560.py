from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("started_at").desc())

latest = (
    df_sessions
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

remaining_keys = (
    latest
    .select("browser", "id")
    .exceptAll(
        df_sessions
        .filter(F.col("browser") != "Chrome")
        .select("browser", "id")
    )
)

kept_rows = (
    latest
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.browser") == F.col("k.browser")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    kept_rows
    .filter(F.col("country").isNull())
    .select(
        "id",
        "customer_id",
        "started_at",
        "rn",
        F.col("rn").cast("string").alias("rn_str"),
    )
)

with_dims = (
    valid
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select("src.*", F.col("c.segment"), F.col("c.credit_limit"), F.col("c.name"))
)

result = (
    with_dims
    .groupBy("customer_id")
    .pivot("segment", ["Consumer", "Corporate", "SMB"])
    .agg(F.sum("credit_limit").alias("total"), F.count(F.lit(1)).alias("item_count"))
    .select(
        "customer_id",
        F.col("Consumer_total").alias("consumer_total"),
        F.col("Consumer_item_count").alias("consumer_item_count"),
        F.col("Corporate_total").alias("corporate_total"),
        F.col("Corporate_item_count").alias("corporate_item_count"),
        F.col("SMB_total").alias("smb_total"),
        F.col("SMB_item_count").alias("smb_item_count"),
    )
)
