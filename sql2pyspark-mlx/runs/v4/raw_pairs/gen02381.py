from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.orderBy("age", "id").rowsBetween(-2, Window.currentRow)
w_part = Window.partitionBy("country").orderBy("age", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_customers
    .select(
        "*",
        F.min("credit_limit").over(w).alias("city_min_credit_limit"),
        F.avg("credit_limit").over(w2).alias("credit_limit_avg_2"),
        F.avg("credit_limit").over(w_part).alias("credit_limit_avg_2_2"),
    )
)
