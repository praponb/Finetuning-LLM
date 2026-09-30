from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("month", "signup_date"), "country").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city")

base = (
    df_customers
    .filter(F.col("city") == "Chiang Mai")
    .select(
        "*",
        F.substring("name", 1, 4).alias("name_prefix"),
        F.coalesce("segment", F.lit("DIRECT")).alias("resolved_segment"),
    )
)

result = (
    base
    .filter(F.col("email").isNotNull())
    .select(
        "id",
        "email",
        "name_prefix",
        F.last("age", ignorenulls=True).over(w).alias("last_known_age"),
        F.round((F.col("age") / F.sum("age").over(w2)), 4).alias("age_share"),
    )
)
