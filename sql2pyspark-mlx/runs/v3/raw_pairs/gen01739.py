from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("hire_date").desc(), "id")
w2 = Window.partitionBy("manager_id")

result = (
    df_employees
    .select(
        "id",
        "manager_id",
        "hire_date",
        F.ntile(10).over(w).alias("quartile"),
        F.count("*").over(w2).alias("manager_id_count"),
    )
)
