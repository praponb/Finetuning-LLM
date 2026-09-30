from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("hire_date", "id")
w2 = Window.partitionBy(F.date_trunc("YEAR", "hire_date"), "manager_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("hire_date").desc(), "id")
w4 = Window.partitionBy(F.month("hire_date"), "level").orderBy("salary")

result = (
    df_employees
    .select(
        "id",
        "manager_id",
        "job_title",
        "level",
        F.row_number().over(w).alias("row_seq"),
        F.first("level", ignorenulls=True).over(w2).alias("first_known_level"),
        F.row_number().over(w3).alias("event_seq"),
        F.cume_dist().over(w4).alias("salary_cume_dist"),
    )
)
