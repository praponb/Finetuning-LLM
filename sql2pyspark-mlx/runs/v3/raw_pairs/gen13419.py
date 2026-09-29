from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "segment").orderBy("age", "id")

result = (
    df_customers
    .filter(F.month("signup_date").isin([1, 2, 3]))
    .select("id", "city", "segment", "age", F.ntile(2).over(w).alias("bucket"))
)
