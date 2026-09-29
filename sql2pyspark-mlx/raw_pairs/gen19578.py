from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("customer_age", "id").rowsBetween(-6, Window.currentRow)

cleaned = (
    df_orders
    .filter((~F.col("channel").eqNullSafe(F.lit("mobile"))) & (F.col("order_date") >= F.lit("2023-09-07").cast("date")))
    .select(
        "id",
        "customer_id",
        "order_date",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.round("amount", 1).alias("amount_rounded"),
        F.concat_ws("-", "channel", "status").alias("channel_status_key"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_clean"),
    )
)

cleaned_orders = (
    cleaned
    .filter((F.col("order_date") > F.lit("2023-09-08").cast("date")) & F.col("channel").isNotNull())
    .select(
        "*",
        F.round((F.col("amount_rounded") * 100), 2).alias("adjusted_amount_rounded"),
        F.add_months("order_date", 1).alias("order_date_plus_months"),
    )
)

joined = (
    cleaned_orders
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id").eqNullSafe(F.col("b.id")), "left")
    .filter(F.col("b.city") == "Singapore")
    .select("src.*", F.col("b.age"), F.col("b.country"), F.col("b.credit_limit"))
)

joined_orders = (
    joined
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "src.id",
        "src.customer_id",
        "src.channel",
        "src.amount",
        "src.discount",
        "src.shipping_fee",
        "src.amount_rounded",
        "src.channel_clean",
        "src.adjusted_amount_rounded",
        "src.order_date_plus_months",
        "src.age",
        F.col("b.country").alias("customer_country"),
        F.col("b.credit_limit").alias("customer_credit_limit"),
    )
)

enriched = (
    joined_orders
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id").eqNullSafe(F.col("c.id")), "left")
    .select(
        "src.id",
        "src.customer_id",
        "src.channel",
        "src.amount_rounded",
        "src.channel_clean",
        "src.order_date_plus_months",
        "src.age",
        F.col("c.name"),
        F.col("c.signup_date"),
        F.col("c.age").alias("customer_age"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "customer_id",
        "channel",
        "amount_rounded",
        "channel_clean",
        "order_date_plus_months",
        "age",
        F.count("amount_rounded").over(w).alias("last7_count_amount_rounded"),
        F.avg("amount_rounded").over(w).alias("last7_avg_amount_rounded"),
        F.max("amount_rounded").over(w).alias("last7_max_amount_rounded"),
    )
)

with_dims = (
    scored
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.age").alias("customer_age"))
)

result = (
    with_dims
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("channel") == "partner", F.col("last7_max_amount_rounded"))).alias("partner_total_last7_max_amount_rounded"),
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_count"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_rows"),
    )
    .filter(F.col("partner_total_last7_max_amount_rounded") < 1000000)
)
