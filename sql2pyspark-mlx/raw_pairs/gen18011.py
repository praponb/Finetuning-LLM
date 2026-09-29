from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.partitionBy("city").orderBy(F.col("credit_limit").desc())
w3 = Window.orderBy("age", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_customers
    .select(
        "id",
        "name",
        "city",
        "is_active",
        "age",
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w)), 4).alias("credit_limit_share"),
        F.rank().over(w2).alias("credit_limit_rank"),
        F.avg("credit_limit").over(w3).alias("credit_limit_avg_3"),
    )
)
