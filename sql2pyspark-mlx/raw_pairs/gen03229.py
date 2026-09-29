from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("age")

flattened = (
    df_carts
    .withColumn("line", F.explode("items"))
    .select("id", "customer_id", "created_at", F.col("line.price"), F.col("line.promo"))
)

with_dims = (
    flattened
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "inner")
    .select("a.*", F.col("b.signup_date"), F.col("b.email"), F.col("b.age"))
)

cleaned = (
    with_dims
    .filter(F.col("customer_id").isNotNull() & (F.col("customer_id") == 32))
    .select(
        "customer_id",
        "price",
        "promo",
        "signup_date",
        "email",
        "age",
        F.initcap(F.trim("promo")).alias("promo_title"),
        F.date_trunc("year", "created_at").alias("created_at_period"),
    )
)

enriched = cleaned.withColumn("age_cume_dist", F.cume_dist().over(w))

with_dims_carts = (
    enriched
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.segment"), F.col("b.name"), F.col("b.city"))
)

with_dims_carts_2 = (
    with_dims_carts
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id").eqNullSafe(F.col("c.id")), "left")
    .select(
        "a.customer_id",
        "a.email",
        "a.name",
        F.col("c.country"),
        F.col("c.name").alias("customer_name"),
    )
)

joined = (
    with_dims_carts_2
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .select(
        "s.*",
        F.col("p.country").alias("customer_country"),
        F.col("p.email").alias("customer_email"),
    )
)

result = (
    joined
    .select(
        "customer_id",
        "email",
        "name",
        "customer_name",
        "customer_country",
        "customer_email",
        F.when(F.col("customer_name").rlike("^(bob smith|David Lee )$"), "CORE").otherwise("EXTENDED").alias("customer_name_zone"),
    )
    .orderBy(F.col("customer_id").asc_nulls_last())
)
