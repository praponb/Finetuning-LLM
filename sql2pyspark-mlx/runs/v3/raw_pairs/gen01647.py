from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sale_cohort")

result = (
    df_store_sales
    .groupBy(
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
    )
    .agg(
        F.sum("units").alias("total_units"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_units", F.sum("total_units").over(w))
    .orderBy(F.col("sale_cohort").desc(), F.col("cnt").desc())
)
