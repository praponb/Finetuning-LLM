from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("age", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("segment").orderBy("signup_date", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("city")

result = (
    df_customers
    .select(
        "*",
        F.avg("age").over(w).alias("age_avg_2"),
        F.avg("age").over(w2).alias("age_avg_3_prior"),
        F.corr("age", "credit_limit").over(w3).alias("corr_age_credit_limit"),
    )
)
