from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("country").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("segment")
w3 = Window.partitionBy("segment").orderBy(F.col("age").desc(), "id")

result = (
    df_customers
    .select(
        "id",
        "email",
        "country",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.last("city", ignorenulls=True).over(w_running).alias("last_known_city"),
        F.count("age").over(w2).alias("segment_count_age"),
        F.ntile(5).over(w3).alias("age_bucket"),
    )
)
