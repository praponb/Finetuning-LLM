from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("salary")
w2 = Window.partitionBy("level").orderBy(F.col("salary").desc(), "id")

result = (
    df_employees
    .filter(F.col("manager_id").isin([2, 12, 15, 34]))
    .select(
        "id",
        "level",
        "salary",
        F.dense_rank().over(w).alias("dense_rnk"),
        F.ntile(4).over(w2).alias("bucket"),
    )
)
