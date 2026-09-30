from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_age").desc())

result = (
    df_customers
    .groupBy(
        F.when(F.col("country").isin(["UK", "US"]), "group_a").otherwise("other").alias("country_group"),
    )
    .agg(F.sum("age").alias("total_age"))
    .withColumn("age_rank", F.rank().over(w))
    .orderBy(F.col("total_age").desc(), "country_group")
    .limit(5)
)
