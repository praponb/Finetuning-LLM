from pyspark.sql import functions as F

flattened = df_carts.select("id", "customer_id", "created_at", F.inline("items"))

with_dims = (
    flattened
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select(
        "e.customer_id",
        "e.created_at",
        "e.price",
        F.col("d.email"),
        F.col("d.age"),
    )
)

filtered = (
    with_dims
    .filter(F.col("email").like("%@startup.io"))
    .select(
        "customer_id",
        "created_at",
        "price",
        "email",
        "age",
        F.length("email").alias("email_length"),
        F.datediff(F.current_date(), "created_at").alias("days_since_created_at"),
    )
)

joined = (
    filtered
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.city"), F.col("b.signup_date"))
)

joined_carts = (
    joined
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .select(
        "s.customer_id",
        "s.email",
        "s.email_length",
        "s.days_since_created_at",
        "s.city",
        F.col("p.email").alias("customer_email"),
    )
)

result = (
    joined_carts
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "inner")
    .cube(F.col("d.segment").alias("segment"), F.col("s.city").alias("cart_city"))
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.col("s.email_length")).alias("total_email_length"),
        F.sum(F.col("s.days_since_created_at")).alias("total_days_since_created_at"),
        F.grouping_id().alias("grouping_level"),
    )
)
