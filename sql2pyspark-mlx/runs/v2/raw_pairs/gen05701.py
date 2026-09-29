from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("city").orderBy(F.col("age").desc())

result = (
    df_customers
    .filter(F.col("credit_limit") >= 13400)
    .select(
        "id",
        "city",
        "age",
        F.round((F.col("age") / F.sum("age").over(w)), 3).alias("age_share"),
        F.rank().over(w2).alias("rnk"),
    )
    .orderBy("age")
)
