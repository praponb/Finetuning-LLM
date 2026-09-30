from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("WEEK", "signup_date"), "segment").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy(F.month("signup_date"), "city").orderBy("signup_date", "id")

enriched = (
    df_customers
    .select(
        "id",
        "email",
        "is_active",
        "credit_limit",
        "age",
        F.last("age", ignorenulls=True).over(w).alias("last_known_age"),
        F.lag("age", 1).over(w_recent).alias("prev_age"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("credit_limit") >= 17700, "HIGH").otherwise("LOW").alias("credit_limit_tier"),
    )
    .agg(
        F.avg("age").alias("avg_age"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("prev_age"), 2).alias("avg_prev_age"),
        F.bit_and("age").alias("age_bit_and"),
        F.round(F.avg("age"), 2).alias("avg_age_2"),
    )
    .filter(F.col("avg_age_2") >= 0)
    .orderBy(F.col("credit_limit_tier").asc_nulls_last())
)
