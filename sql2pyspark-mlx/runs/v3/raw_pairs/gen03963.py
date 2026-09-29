from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_units").desc())

result = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 3]))
    .groupBy(
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
    )
    .agg(F.sum("units").alias("total_units"))
    .withColumn("units_rank", F.rank().over(w))
    .orderBy("total_units", "sale_cohort")
    .limit(1)
)
