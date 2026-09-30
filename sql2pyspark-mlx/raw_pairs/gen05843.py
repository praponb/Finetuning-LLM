from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("code"),
    )
)

enriched = (
    flattened
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "inner")
    .select("src.id", "src.customer_id", F.col("d.name"), F.col("d.email"))
)

base = (
    enriched
    .filter(~F.col("name").like("Ja%"))
    .withColumn("clean_name", F.trim("name"))
)

enriched_carts = (
    base
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .filter(F.col("b.city").isNotNull())
    .select(
        "e.id",
        "e.customer_id",
        "e.name",
        "e.clean_name",
        F.col("b.email").alias("customer_email"),
        F.col("b.credit_limit"),
    )
)

enriched_carts_2 = (
    enriched_carts
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.name").alias("customer_name"))
)

with_dims = (
    enriched_carts_2
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id").eqNullSafe(F.col("d.id")), "left")
    .select(
        "s.id",
        "s.customer_id",
        "s.name",
        "s.customer_email",
        "s.credit_limit",
        F.col("d.name").alias("customer_name_2"),
    )
)

result = with_dims.select("customer_id", "customer_email", "credit_limit")
