from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("signup_date", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_customers
    .select(
        "id",
        "country",
        "signup_date",
        "age",
        F.round(F.avg("age").over(w), 2).alias("age_moving_avg_7"),
    )
    .orderBy("id")
)
