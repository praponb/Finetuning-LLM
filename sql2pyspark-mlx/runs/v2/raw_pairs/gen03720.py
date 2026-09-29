from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment")

result = (
    df_customers
    .filter(F.col("city").isin(["Chiang Mai", "New York", "Singapore"]))
    .select(
        "id",
        "segment",
        "age",
        F.round((F.col("age") / F.sum("age").over(w)), 3).alias("age_share"),
    )
    .orderBy(F.col("id").desc(), F.col("age").desc())
)
