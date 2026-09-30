from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("city").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("segment").orderBy("credit_limit", "id")

result = (
    df_customers
    .select(
        "id",
        "email",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        F.avg("age").over(w_recent).alias("running_avg_age"),
        F.max("age").over(w_recent).alias("running_max_age"),
        F.count("age").over(w_recent).alias("running_count_age"),
        F.ntile(5).over(w_ordered).alias("credit_limit_bucket"),
    )
)
