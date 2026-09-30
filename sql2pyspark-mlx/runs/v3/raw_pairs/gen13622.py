from pyspark.sql import functions as F

result = df_customers.filter(F.upper("name").like("%OB %"))
