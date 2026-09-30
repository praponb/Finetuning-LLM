from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").asc_nulls_first(), "id")

filtered = (
    df_departments
    .filter(F.col("location").isNotNull() & (~F.col("location").eqNullSafe(F.lit("Bangkok"))))
    .withColumn("dept_name_upper", F.upper("dept_name"))
)

result = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "dept_name_upper")
)
