from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("segment")

scored = (
    df_customers
    .filter(F.col("signup_date") <= "2025-07-01")
    .select(
        "id",
        "email",
        "country",
        "segment",
        F.min("credit_limit").over(w).alias("running_min_credit_limit"),
        F.avg("credit_limit").over(w).alias("running_avg_credit_limit"),
        F.max("credit_limit").over(w).alias("running_max_credit_limit"),
        F.count("credit_limit").over(w).alias("running_count_credit_limit"),
        F.round((F.col("age") / F.sum("age").over(w2)), 4).alias("age_share"),
    )
)

result = (
    scored
    .select(
        "id",
        "email",
        "running_min_credit_limit",
        "running_avg_credit_limit",
        "age_share",
    )
    .orderBy(F.col("id").asc_nulls_last())
    .limit(5)
)
