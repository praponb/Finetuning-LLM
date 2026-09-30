from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("country").orderBy("age", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_customers
    .filter(F.col("email").like("%@mail.co.th"))
    .select(
        "id",
        "country",
        "city",
        "signup_date",
        "is_active",
        "credit_limit",
        F.max("age").over(w_running).alias("last3_max_age"),
        F.count("age").over(w_running).alias("last3_count_age"),
        F.sum("age").over(w_running).alias("last3_sum_age"),
    )
)
