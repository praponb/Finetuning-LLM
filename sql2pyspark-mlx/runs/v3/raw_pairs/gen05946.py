from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("salary").desc(), "id")

result = (
    df_employees
    .filter(F.col("email").like("%@company.co"))
    .select("id", "salary", "level", F.first("level").over(w).alias("first_level"))
)
