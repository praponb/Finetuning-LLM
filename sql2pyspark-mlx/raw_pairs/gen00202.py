from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget_filled", "id")

cleaned = (
    df_departments
    .filter(~F.col("dept_name").eqNullSafe(F.lit("Marketing")))
    .withColumn("dept_name_group", F.when(F.col("dept_name").isin(["Legal", "Finance"]), "Priority").otherwise("Other"))
    .withColumn("budget_filled", F.coalesce("budget", F.lit(0)))
)

result = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)
