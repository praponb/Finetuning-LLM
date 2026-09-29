from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("WEEK", "signup_date"), "country").orderBy("signup_date", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_customers
    .filter(F.col("city") != "New York")
    .select(
        "id",
        "signup_date",
        "country",
        "age",
        F.round(F.avg("age").over(w), 2).alias("age_moving_avg_4"),
    )
)
