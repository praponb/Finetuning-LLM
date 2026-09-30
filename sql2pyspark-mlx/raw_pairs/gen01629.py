from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("value").orderBy(F.col("created_at_quarter").desc())
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.avg("balance").alias("avg_account_balance"))

lines = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("coupon_codes").alias("value"))
)

base = (
    lines
    .filter(F.col("created_at") > F.lit("2025-03-01").cast("date"))
    .select(
        "*",
        F.length("value").alias("value_length"),
        F.quarter("created_at").alias("created_at_quarter"),
        F.coalesce("value", F.lit("N/A")).alias("value_filled"),
    )
)

enriched = (
    base
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select("src.*", F.col("c.country"), F.col("c.name"), F.col("c.segment"))
)

with_windows = enriched.withColumn("created_at_quarter_rank", F.rank().over(w))

result = (
    with_windows
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "created_at",
        "value_filled",
        "country",
        "name",
        "segment",
        "created_at_quarter_rank",
        F.when(F.col("segment").rlike("^(SMB|Consumer)$"), "GROUP_A").when(F.col("segment").rlike("^(Corporate)$"), "PRIORITY").otherwise("EXTENDED").alias("segment_zone"),
        F.col("avg_account_balance"),
    )
)
