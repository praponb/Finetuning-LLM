from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("signup_date", "id")
w2 = Window.partitionBy("segment").orderBy("signup_date", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_customers
    .select(
        "id",
        "country",
        "signup_date",
        "segment",
        "age",
        F.ntile(3).over(w).alias("signup_date_ntile"),
        F.round(F.avg("age").over(w2), 2).alias("age_moving_avg_4"),
    )
    .orderBy(F.col("signup_date").desc(), F.col("age_moving_avg_4").desc())
)
