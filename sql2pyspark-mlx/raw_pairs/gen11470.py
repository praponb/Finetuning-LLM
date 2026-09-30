from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_products
    .filter(F.col("category").isin(["Sports", "Grocery"]))
    .groupBy(
        F.when(F.year("launch_date") < 2024, "before_2024").when(F.year("launch_date") == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
    )
    .agg(F.sum("cost").alias("total_cost"))
    .withColumn("pct_of_total", F.round(((F.col("total_cost") * 100) / F.sum("total_cost").over(w)), 2))
    .orderBy("total_cost", "launch_cohort")
)
