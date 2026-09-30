from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location")
w2 = Window.partitionBy("location", "dept_name").orderBy("is_london", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("is_london", F.when(F.col("location") == "London", 1).otherwise(0))
    .withColumn("resolved_dept_name", F.coalesce("dept_name", F.lit("UNKNOWN")))
)

result = (
    base
    .filter(F.col("location") == "Tokyo")
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        F.stddev_samp("is_london").over(w).alias("dept_name_stddev_samp_is_london"),
        F.sum("is_london").over(w2).alias("running_is_london"),
    )
)
