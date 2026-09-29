from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("segment").orderBy("signup_date", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_customers
    .filter(F.col("signup_date") <= "2024-08-01")
    .select(
        "id",
        "city",
        "age",
        "segment",
        "signup_date",
        "credit_limit",
        F.round((F.col("age") / F.sum("age").over(w)), 3).alias("age_share"),
        F.round(F.avg("credit_limit").over(w2), 2).alias("credit_limit_moving_avg_4"),
    )
)
