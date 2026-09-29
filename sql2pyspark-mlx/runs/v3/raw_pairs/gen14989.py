from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_orders
    .groupBy(
        F.when(F.year("order_date") < 2024, "before_2024").when(F.year("order_date") == 2024, "2024").otherwise("after_2024").alias("order_cohort"),
    )
    .agg(F.sum("discount").alias("total_discount"))
    .withColumn("pct_of_total", F.round(((F.col("total_discount") * 100) / F.sum("total_discount").over(w)), 2))
    .orderBy("total_discount", "order_cohort")
)
