from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("signup_date", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("city").orderBy(F.col("credit_limit").desc(), "id")

result = (
    df_customers
    .filter(F.col("city") == "Berlin")
    .select(
        "id",
        "segment",
        "signup_date",
        "age",
        "city",
        "credit_limit",
        F.round(F.avg("age").over(w), 2).alias("age_moving_avg_4"),
        F.row_number().over(w2).alias("row_num"),
    )
)
