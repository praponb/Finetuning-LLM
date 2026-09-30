from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "country")

lines = (
    df_carts
    .select("id", "customer_id", F.explode_outer("coupon_codes").alias("element"))
)

with_dims = (
    lines
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select("a.id", "a.customer_id", "a.element", F.col("p.country"))
)

filtered = (
    with_dims
    .filter(F.col("customer_id").isNull())
    .withColumn("is_spring10", F.when(F.col("element") == "SPRING10", 1).otherwise(0))
    .withColumn("element_prefix", F.substring("element", 1, 2))
    .withColumn("resolved_country", F.coalesce("country", F.lit("OTHER")))
)

with_dims_carts = (
    filtered
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .select(
        "s.customer_id",
        "s.country",
        "s.is_spring10",
        "s.element_prefix",
        F.col("b.age"),
    )
)

with_dims_carts_2 = (
    with_dims_carts
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id").eqNullSafe(F.col("d.id")), "left")
    .select("src.*", F.col("d.email"), F.col("d.name"))
)

kept_keys = (
    with_dims_carts_2
    .select("customer_id")
    .intersect(
        df_carts
        .filter(F.array_contains("coupon_codes", "WELCOME5"))
        .select("customer_id")
    )
)

kept_rows = (
    with_dims_carts_2
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .select("*", F.min("is_spring10").over(w).alias("customer_id_min_is_spring10"))
)

result = (
    scored
    .select(
        "customer_id",
        "country",
        "age",
        "name",
        "customer_id_min_is_spring10",
        F.when(F.col("name").rlike("^(FRANK MILLER|bob smith)$"), "PRIORITY").when(F.col("name").rlike("^(Alice Wong|  Carol Diaz)$"), "CORE").otherwise("EXTENDED").alias("name_zone"),
    )
    .orderBy(F.col("country").desc())
)
