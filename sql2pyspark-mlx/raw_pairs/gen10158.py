from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country")
w2 = Window.partitionBy("country").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("segment", "city").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.year("signup_date"), "country").orderBy("credit_limit", "id").rowsBetween(-2, -1)
w5 = Window.orderBy(F.col("age").desc_nulls_last())

base = (
    df_customers
    .filter(~F.col("email").like("%@company.co") & (~F.col("city").eqNullSafe(F.lit("New York"))))
    .select(
        "*",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
        F.coalesce("city", F.lit("NONE")).alias("resolved_city"),
    )
)

result = (
    base
    .select(
        "id",
        "name",
        "email",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "is_active_label",
        F.round((F.col("age") / F.sum("age").over(w)), 4).alias("age_share"),
        F.last("age", ignorenulls=True).over(w2).alias("last_known_age"),
        F.last("city", ignorenulls=True).over(w3).alias("last_known_city"),
        F.avg("credit_limit").over(w4).alias("credit_limit_avg_2_prior"),
        F.cume_dist().over(w5).alias("age_cume_dist"),
    )
)
