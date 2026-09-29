from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id")
w2 = Window.partitionBy("level").orderBy(F.col("salary").desc(), "id")

result = (
    df_employees
    .filter(F.col("email").isNotNull())
    .select(
        "id",
        "dept_id",
        "salary",
        "level",
        F.avg("salary").over(w).alias("dept_id_avg_salary"),
        F.ntile(3).over(w2).alias("quartile"),
    )
    .orderBy("quartile")
)
