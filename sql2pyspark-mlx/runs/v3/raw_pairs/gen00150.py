from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_stores
    .groupBy(
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(F.sum("sqft").alias("total_sqft"))
    .withColumn("pct_of_total", F.round(((F.col("total_sqft") * 100) / F.sum("total_sqft").over(w)), 2))
)
