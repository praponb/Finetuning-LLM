from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")

result = (
    df_employees
    .filter(F.col("level") == "L2")
    .select(
        "id",
        "level",
        "salary",
        F.round((F.col("salary") / F.sum("salary").over(w)), 3).alias("salary_share"),
    )
)
