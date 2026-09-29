from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_suppliers
    .filter(F.col("rating").between(2.1, 4.2))
    .select("id", "city", "rating", F.sum("rating").over(w).alias("running_rating"))
)
