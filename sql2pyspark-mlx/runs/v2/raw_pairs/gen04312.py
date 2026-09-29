from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_suppliers
    .select("id", "country", "rating", F.sum("rating").over(w).alias("running_rating"))
    .orderBy("rating")
)
