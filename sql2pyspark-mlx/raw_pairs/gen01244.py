from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")

result = df_suppliers.select("id", "country", "rating", F.ntile(4).over(w).alias("quartile"))
