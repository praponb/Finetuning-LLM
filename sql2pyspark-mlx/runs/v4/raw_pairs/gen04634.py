from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("credit_limit").asc_nulls_first())
w2 = Window.partitionBy("city").orderBy("credit_limit", "id").rowsBetween(-6, -1)
w3 = Window.orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_customers
    .filter((F.col("signup_date") > F.lit("2023-06-25").cast("date")) & F.col("city").isin(["Bangkok", "London"]))
    .select("*", F.upper("segment").alias("segment_upper"))
)

result = (
    valid
    .select(
        "*",
        F.rank().over(w).alias("credit_limit_rank"),
        F.avg("credit_limit").over(w2).alias("credit_limit_avg_6_prior"),
        F.first("credit_limit", ignorenulls=True).over(w3).alias("first_known_credit_limit"),
    )
)
