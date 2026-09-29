from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("country").orderBy("rating", "id")

result = (
    df_suppliers
    .withColumn("prev_rating", F.lag("rating").over(w_ordered))
    .withColumn("row_seq", F.row_number().over(w_ordered))
)
