from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy(F.col("budget").desc_nulls_first(), "id")

cleaned = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .select(
        "*",
        F.when(F.col("location").isin(["Tokyo", "Chiang Mai"]), "Tier 1").otherwise("Other").alias("location_group"),
        F.coalesce("location", F.lit("DIRECT")).alias("location_filled"),
    )
)

result = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "dept_name", "location", "location_group")
)
