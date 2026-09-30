from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_discount").desc())

result = (
    df_orders
    .groupBy(
        F.when(F.year("order_date") < 2024, "before_2024").when(F.year("order_date") == 2024, "2024").otherwise("after_2024").alias("order_cohort"),
    )
    .agg(F.sum("discount").alias("total_discount"))
    .withColumn("discount_rank", F.dense_rank().over(w))
    .orderBy(F.col("discount_rank").desc())
)
