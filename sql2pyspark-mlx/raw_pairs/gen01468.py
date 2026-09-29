from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("salary")

ranked = (
    df_employees
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "full_name", "hire_date", "level")
)

result = (
    ranked
    .select("id")
    .orderBy(F.col("id").desc())
)
