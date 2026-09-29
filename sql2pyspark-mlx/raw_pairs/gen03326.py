from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("salary").desc(), "id")
w2 = Window.partitionBy(F.month("hire_date"), "level")
w3 = Window.partitionBy("dept_id", "manager_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("dept_id", "manager_id").orderBy("salary")
w5 = Window.partitionBy(F.date_trunc("MONTH", "hire_date"), "level")

result = (
    df_employees
    .filter(F.col("dept_id").isin([22, 33]))
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("salary_share", F.round((F.col("salary") / F.sum("salary").over(w2)), 4))
    .withColumn("last_known_level", F.last("level", ignorenulls=True).over(w3))
    .withColumn("salary_dense_rank", F.dense_rank().over(w_grp))
    .withColumn("hire_date_period_stddev_samp_salary", F.stddev_samp("salary").over(w5))
)
