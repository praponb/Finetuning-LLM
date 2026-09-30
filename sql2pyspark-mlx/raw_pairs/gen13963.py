from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy(F.year("signup_date"), "country").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.month("signup_date"), "city").orderBy("credit_limit", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("country").orderBy("credit_limit")
w4 = Window.partitionBy("country").orderBy(F.col("credit_limit").asc_nulls_last())
w5 = Window.partitionBy("segment").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_customers
    .filter(F.col("segment") == "SMB")
    .select(
        "id",
        "email",
        "country",
        "city",
        "segment",
        "is_active",
        "age",
        F.sum("age").over(w_running).alias("running_age"),
        F.avg("age").over(w2).alias("age_avg_5_prior"),
        F.cume_dist().over(w3).alias("credit_limit_cume_dist"),
        F.rank().over(w4).alias("credit_limit_rank"),
        F.last("credit_limit", ignorenulls=True).over(w5).alias("last_known_credit_limit"),
    )
)
