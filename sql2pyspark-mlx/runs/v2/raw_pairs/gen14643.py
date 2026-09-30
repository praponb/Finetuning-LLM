from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("signup_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w2 = Window.partitionBy("city").orderBy("signup_date", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_customers
    .filter(F.col("country") == "DE")
    .select(
        "id",
        "segment",
        "signup_date",
        "credit_limit",
        "city",
        F.last("credit_limit").over(w).alias("last_credit_limit"),
        F.round(F.avg("credit_limit").over(w2), 2).alias("credit_limit_moving_avg_3"),
    )
)
