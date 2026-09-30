from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_sessions
    .filter(F.col("country") == "JP")
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        F.lower("country").alias("country_lower"),
    )
)

scored = (
    cleaned
    .filter(F.col("device") != "desktop")
    .select(
        "id",
        "customer_id",
        "device",
        "country",
        "country_lower",
        F.first("country", ignorenulls=True).over(w).alias("first_known_country"),
        F.last("country", ignorenulls=True).over(w).alias("last_known_country"),
    )
)

joined = (
    scored
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "s.id",
        "s.customer_id",
        "s.country",
        "s.last_known_country",
        F.col("p.country").alias("customer_country"),
    )
)

with_dims = (
    joined
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.last_known_country",
        "e.customer_country",
        F.col("d.credit_limit"),
        F.col("d.city"),
    )
)

base = (
    with_dims
    .filter(F.col("city").isNotNull())
    .withColumn("is_sg", F.when(F.col("customer_country") == "SG", 1).otherwise(0))
    .withColumn("customer_country_length", F.length("customer_country"))
)

result = (
    base
    .select(
        "id",
        "customer_id",
        "last_known_country",
        "customer_country",
        "credit_limit",
        "customer_country_length",
    )
)
