from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("salary")
w2 = Window.partitionBy("level")

result = (
    df_employees
    .select(
        "id",
        "dept_id",
        "salary",
        "level",
        F.rank().over(w).alias("rnk"),
        F.count("*").over(w2).alias("level_count"),
    )
)
