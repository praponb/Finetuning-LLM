from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy("signup_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("segment").orderBy(F.col("credit_limit").desc(), "id")

result = (
    df_customers
    .filter(F.upper("name").like("%ACK%"))
    .select(
        "id",
        "city",
        "country",
        "signup_date",
        "age",
        "segment",
        "credit_limit",
        F.round(F.avg("age").over(w), 2).alias("age_moving_avg_7"),
        F.ntile(4).over(w2).alias("bucket"),
    )
    .orderBy("signup_date")
)
