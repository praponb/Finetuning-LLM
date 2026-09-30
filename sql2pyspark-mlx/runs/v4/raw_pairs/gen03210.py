from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").desc_nulls_first())
w2 = Window.partitionBy("location").orderBy("rn", "id").rowsBetween(-2, Window.currentRow)
w_ordered = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").desc(), "id")
w4 = Window.partitionBy("dept_name")

valid = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("location_prefix", F.substring("location", 1, 3))
)

latest = (
    valid
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

metrics = (
    latest
    .select(
        "*",
        F.avg("budget").over(w2).alias("budget_avg_2"),
        F.ntile(3).over(w_ordered).alias("budget_bucket"),
        F.count("budget").over(w4).alias("dept_name_count_budget"),
    )
)

result = (
    metrics
    .groupBy("dept_name")
    .pivot("location", ["Singapore", "Berlin"])
    .agg(F.sum("budget").alias("amount"), F.count(F.lit(1)).alias("item_count"))
)
