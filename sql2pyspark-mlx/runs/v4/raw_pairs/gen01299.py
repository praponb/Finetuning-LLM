from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("salary", "id")
w2 = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_employees
    .select(
        "id",
        "level",
        "salary",
        "hire_date",
        F.ntile(4).over(w).alias("salary_ntile"),
        F.round(F.avg("salary").over(w2), 2).alias("salary_moving_avg_7"),
    )
)
