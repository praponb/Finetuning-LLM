from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("attributes").alias("attr_key", "value"),
    )
)

with_dims = (
    lines
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .filter(F.col("b.segment") == "SMB")
    .select(
        "s.id",
        "s.customer_id",
        "s.created_at",
        "s.attr_key",
        F.col("b.signup_date"),
    )
)

joined = (
    with_dims
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.city"), F.col("d.credit_limit"))
)

joined_carts = (
    joined
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.age"), F.col("p.country"))
)

result = (
    joined_carts
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "inner")
    .groupingSets(
        [["segment", "customer_country"], ["segment"], []],
        F.col("c.segment").alias("segment"),
        F.col("c.country").alias("customer_country"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("src.age")).alias("max_age"),
    )
)
