from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("country")

flattened = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("coupon_codes").alias("tag"))
)

with_dims = (
    flattened
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.country"), F.col("p.signup_date"), F.col("p.age"))
)

metrics = with_dims.select("*", F.sum("age").over(w_running).alias("country_sum_age"))

cleaned = (
    metrics
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "created_at",
        "signup_date",
        (F.datediff(F.current_date(), "created_at") / 7).alias("weeks_since_created_at"),
    )
)

grouped = (
    cleaned
    .groupBy(F.date_trunc("week", "signup_date").alias("week_start"), "customer_id")
    .agg(
        F.max("created_at").alias("last_seen"),
        F.percentile_approx("weeks_since_created_at", 0.5).alias("weeks_since_created_at_p50"),
    )
    .filter(F.col("weeks_since_created_at_p50") >= 0)
)

result = (
    grouped
    .select("week_start", "customer_id", "last_seen")
    .orderBy(F.col("customer_id").desc_nulls_last())
)
