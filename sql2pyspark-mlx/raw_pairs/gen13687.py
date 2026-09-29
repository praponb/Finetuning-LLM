from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("salary").desc(), "id")

filtered = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-04-27").cast("date")) & F.col("manager_id").isNotNull())
    .select(
        "*",
        F.last_day("hire_date").alias("hire_date_month_end"),
        F.coalesce("level", F.lit("UNKNOWN")).alias("level_clean"),
    )
)

result = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)
