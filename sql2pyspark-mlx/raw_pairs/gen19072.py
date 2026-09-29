from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("start_date").desc(), "id")

base = (
    df_subscriptions
    .filter(F.col("start_date") >= F.to_date(F.lit("2023-07-14")))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "status",
        F.lower("status").alias("status_lower"),
    )
)

latest = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "customer_id", "status_lower")
)

enriched = (
    latest
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.name"))
)

base_subscriptions = (
    enriched
    .filter(F.col("customer_id").isNotNull())
    .select("*", F.lower("name").alias("name_lower"))
)

result = base_subscriptions.select("id", "name_lower")
