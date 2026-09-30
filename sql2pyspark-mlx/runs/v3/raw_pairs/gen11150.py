from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("hire_date").desc())

result = (
    df_employees
    .filter(F.col("level") == "L3")
    .select(
        "id",
        "level",
        "hire_date",
        F.dense_rank().over(w).alias("hire_date_dense_rank"),
    )
)
