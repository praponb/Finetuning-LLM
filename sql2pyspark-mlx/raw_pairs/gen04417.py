from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("age", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("country").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country", "city").orderBy(F.col("credit_limit").asc_nulls_first())
w4 = Window.partitionBy("city").orderBy("credit_limit", "id")

scored = (
    df_customers
    .select(
        "id",
        "email",
        "signup_date",
        "is_active",
        F.avg("age").over(w).alias("age_avg_2"),
        F.last("credit_limit", ignorenulls=True).over(w2).alias("last_known_credit_limit"),
        F.percent_rank().over(w3).alias("credit_limit_percent_rank"),
        F.ntile(5).over(w4).alias("credit_limit_bucket"),
    )
)

result = (
    scored
    .select(
        "id",
        "email",
        "signup_date",
        "is_active",
        "age_avg_2",
        "credit_limit_percent_rank",
        "credit_limit_bucket",
    )
)
